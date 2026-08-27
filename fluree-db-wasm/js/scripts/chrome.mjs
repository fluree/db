// Headless Chrome + a minimal DevTools-protocol client, shared by the smoke
// scripts. No puppeteer/webdriver dependency — node ≥22 has a global
// WebSocket, and everything these smokes need is `Runtime.evaluate` on one
// page target.

import { spawn, spawnSync } from "node:child_process";
import { existsSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

/** Locate a Chrome/Chromium binary, or null. */
export function findChrome() {
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

/**
 * Launch headless Chrome on a throwaway profile and wait for its DevTools
 * endpoint. Resolves `{ wsUrl, kill }`; `kill()` is idempotent and never
 * throws (a leftover temp profile must not decide a smoke's exit code).
 */
export function launchChrome(chromeBin, { extraArgs = [], timeoutMs = 30_000 } = {}) {
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
      ...extraArgs,
      "about:blank",
    ],
    { stdio: ["ignore", "ignore", "pipe"] },
  );

  const kill = () => {
    try { chrome.kill("SIGKILL"); } catch { /* already gone */ }
    try {
      // The killed Chrome may still be flushing profile files; retry, and
      // never let cleanup decide the exit code.
      rmSync(profileDir, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
    } catch { /* leftover temp dir is harmless */ }
  };

  const wsUrl = new Promise((resolve, reject) => {
    let buf = "";
    const t = setTimeout(
      () => reject(new Error("Chrome did not announce a DevTools endpoint")),
      timeoutMs,
    );
    chrome.stderr.on("data", (d) => {
      buf += d.toString();
      const m = /DevTools listening on (ws:\/\/\S+)/.exec(buf);
      if (m) { clearTimeout(t); resolve(m[1]); }
    });
    chrome.on("error", (e) => { clearTimeout(t); reject(e); });
    chrome.on("exit", (code) => {
      clearTimeout(t);
      reject(new Error(`Chrome exited early (${code}): ${buf}`));
    });
  });

  return { wsUrl, kill };
}

/** Connect a CDP client to `wsUrl`. Resolves `{ send, close }`. */
export async function cdpConnect(wsUrl) {
  const ws = new WebSocket(wsUrl);
  await new Promise((resolve, reject) => {
    ws.onopen = resolve;
    ws.onerror = reject;
  });
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
  const send = (method, params = {}, sessionId) => {
    const id = nextId++;
    ws.send(JSON.stringify({ id, method, params, sessionId }));
    return new Promise((resolve, reject) => waiting.set(id, { resolve, reject }));
  };
  return { send, close: () => ws.close() };
}

/** Open `url` in a fresh page target. Resolves the CDP session id. */
export async function openPage(cdp, url) {
  const { targetId } = await cdp.send("Target.createTarget", { url: "about:blank" });
  const { sessionId } = await cdp.send("Target.attachToTarget", { targetId, flatten: true });
  await cdp.send("Runtime.enable", {}, sessionId);
  await cdp.send("Page.enable", {}, sessionId);
  await cdp.send("Page.navigate", { url }, sessionId);
  return sessionId;
}

/**
 * Evaluate `expression` in the page and return its value. Throws on a page
 * exception, so a silent `undefined` can never read as success.
 */
export async function evaluate(cdp, sessionId, expression) {
  const { result, exceptionDetails } = await cdp.send(
    "Runtime.evaluate",
    { expression, awaitPromise: true, returnByValue: true },
    sessionId,
  );
  if (exceptionDetails) {
    throw new Error(exceptionDetails.exception?.description ?? exceptionDetails.text ?? "evaluate threw");
  }
  return result.value;
}

/**
 * Await a promise the page published on `window[name]`, polling until the
 * page installs it. A page that never installs the marker fails loudly
 * rather than resolving `undefined` — the ran-marker rule.
 */
export function awaitPageMarker(cdp, sessionId, name, { pollMs = 100, tries = 600 } = {}) {
  return evaluate(
    cdp,
    sessionId,
    `(async () => {
      for (let i = 0; i < ${tries} && !window[${JSON.stringify(name)}]; i++) {
        await new Promise(r => setTimeout(r, ${pollMs}));
      }
      if (!window[${JSON.stringify(name)}]) {
        return { status: "fail", error: "page never installed window.${name}" };
      }
      return await window[${JSON.stringify(name)}];
    })()`,
  );
}
