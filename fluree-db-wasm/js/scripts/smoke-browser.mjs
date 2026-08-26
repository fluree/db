#!/usr/bin/env node
// Browser smoke test of the SHIPPED package: main-thread proxy → worker →
// wasm, in a real Chrome, over plain HTTP with no special headers.
//
//   node scripts/smoke-browser.mjs            # after `node scripts/build.mjs`
//   CHROME=/path/to/chrome node scripts/smoke-browser.mjs
//
// Drives headless Chrome over the DevTools protocol (no puppeteer/webdriver
// dependency — node ≥22 has a global WebSocket). Loads demo/smoke.html, which
// sets `window.__smoke` to a promise; the test awaits it via
// Runtime.evaluate and exits 0 ONLY when the page reports `status: "pass"`
// with the expected bound rows — a positive ran-marker. Timeouts, thrown
// errors, and a missing marker all exit 1.

import { spawn, spawnSync } from "node:child_process";
import { existsSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { packageRoot, startServer } from "./serve.mjs";

const TIMEOUT_MS = Number(process.env.SMOKE_TIMEOUT_MS ?? 180_000);

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

const { server, url } = await startServer(packageRoot, 0);
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
  if (r?.status === "pass" && r.rows === 2 && names === "Alice,Bob" && r.ask === true) {
    console.log(`PASS: create → insert → query returned ${r.rows} rows via ${r.transport} transport ` +
      `(engine ${r.version}; init ${r.timings.init} ms, insert ${r.timings.insert} ms, query ${r.timings.query} ms)`);
    exit = 0;
  } else {
    console.error("FAIL: page did not report the expected rows");
  }
} catch (err) {
  console.error("FAIL:", err.message ?? err);
} finally {
  clearTimeout(deadline);
  ws.close();
  cleanup();
}
process.exit(exit);
