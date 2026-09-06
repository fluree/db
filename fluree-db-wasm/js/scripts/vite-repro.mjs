#!/usr/bin/env node
// Bundler verification for the zero-config claim (PR-1715 review H-6):
// packs the package, scaffolds a minimal Vite app that imports it with no
// configuration, `vite build`s it, asserts the worker chunk AND the .wasm
// asset were statically detected and emitted, then serves the BUILT output
// and runs playground() → insert → query in headless Chrome.
//
//   node scripts/vite-repro.mjs        # after `node scripts/build.mjs`
//
// Dev-box tool, not CI (it npm-installs Vite from the network). Exits 0 only
// on the runtime positive marker from the built app.

import { spawn, spawnSync } from "node:child_process";
import { existsSync, mkdtempSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { packageRoot, startServer } from "./serve.mjs";

function run(cmd, args, opts = {}) {
  const r = spawnSync(cmd, args, { encoding: "utf8", ...opts });
  if (r.status !== 0) {
    console.error(`${cmd} ${args.join(" ")} failed:\n${r.stdout}\n${r.stderr}`);
    process.exit(2);
  }
  return r.stdout;
}

for (const f of ["dist/index.js", "dist/worker.js", "pkg/fluree_db_wasm_bg.wasm"]) {
  if (!existsSync(join(packageRoot, f))) {
    console.error(`missing ${f} — run \`node scripts/build.mjs\` first`);
    process.exit(2);
  }
}

const app = mkdtempSync(join(tmpdir(), "fluree-wasm-vite-"));
console.log(`scaffolding Vite app in ${app}`);
const tarball = run("npm", ["pack", "--pack-destination", app], { cwd: packageRoot })
  .trim()
  .split("\n")
  .pop();
writeFileSync(
  join(app, "package.json"),
  JSON.stringify({ name: "vite-repro", private: true, type: "module" }, null, 2),
);
run("npm", ["install", "--no-audit", "--no-fund", "vite", `./${tarball}`], { cwd: app });
writeFileSync(
  join(app, "index.html"),
  `<!doctype html><meta charset="utf-8"><pre id="out">running…</pre>
<script type="module">
  import { playground } from "@fluree/db-wasm";
  window.__smoke = (async () => {
    try {
      const pg = await playground();
      const ledger = await pg.createLedger("vite");
      await ledger.insert({ "@context": { ex: "http://example.org/ns/" },
        "@id": "ex:v", "@type": "ex:Thing", "ex:name": "vite" });
      const res = await ledger.query(
        'PREFIX ex: <http://example.org/ns/> SELECT ?n WHERE { ?s ex:name ?n }');
      pg.close();
      const names = res.results.bindings.map((b) => b.n.value);
      const report = { status: "pass", rows: names.length, names };
      document.getElementById("out").textContent = JSON.stringify(report);
      return report;
    } catch (err) {
      const report = { status: "fail", error: String(err?.stack ?? err) };
      document.getElementById("out").textContent = JSON.stringify(report);
      return report;
    }
  })();
</script>`,
);
run("npx", ["vite", "build", "--logLevel", "warn"], { cwd: app });

// Static-detection evidence: the worker chunk and the wasm asset must exist
// in the build output — a variable-passed worker URL emits neither.
const assets = readdirSync(join(app, "dist", "assets"));
const wasmAssets = assets.filter((a) => a.endsWith(".wasm"));
const workerAssets = assets.filter((a) => /worker/i.test(a) && a.endsWith(".js"));
console.log(`vite build assets: ${assets.join(", ")}`);
if (wasmAssets.length === 0 || workerAssets.length === 0) {
  console.error(
    `FAIL: static worker detection did not emit the expected assets ` +
      `(wasm: ${wasmAssets.length}, worker chunks: ${workerAssets.length})`,
  );
  process.exit(1);
}

// Runtime proof against the BUILT output.
const { server, url } = await startServer(join(app, "dist"), 0);
const candidates = [
  process.env.CHROME,
  "google-chrome",
  "google-chrome-stable",
  "chromium",
  "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
].filter(Boolean);
let chromeBin = null;
for (const c of candidates) {
  if (c.includes("/") ? existsSync(c) : spawnSync("which", [c], { stdio: "ignore" }).status === 0) {
    chromeBin = c;
    break;
  }
}
if (!chromeBin) {
  console.error("no Chrome found; set CHROME=/path/to/binary");
  process.exit(2);
}
const profileDir = mkdtempSync(join(tmpdir(), "fluree-wasm-vite-chrome-"));
const chrome = spawn(
  chromeBin,
  ["--headless=new", "--disable-gpu", "--no-sandbox", "--no-first-run",
    `--user-data-dir=${profileDir}`, "--remote-debugging-port=0", "about:blank"],
  { stdio: ["ignore", "ignore", "pipe"] },
);
function cleanup() {
  try { chrome.kill("SIGKILL"); } catch { /* gone */ }
  server.close();
  for (const d of [profileDir, app]) {
    try { rmSync(d, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 }); } catch { /* harmless */ }
  }
}
const wsUrl = await new Promise((resolve, reject) => {
  let buf = "";
  const t = setTimeout(() => reject(new Error("no DevTools endpoint")), 30_000);
  chrome.stderr.on("data", (d) => {
    buf += d.toString();
    const m = /DevTools listening on (ws:\/\/\S+)/.exec(buf);
    if (m) { clearTimeout(t); resolve(m[1]); }
  });
  chrome.on("exit", (code) => { clearTimeout(t); reject(new Error(`Chrome exited (${code})`)); });
}).catch((e) => { console.error(e.message); cleanup(); process.exit(1); });

const ws = new WebSocket(wsUrl);
await new Promise((res, rej) => { ws.onopen = res; ws.onerror = rej; });
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
const cdp = (method, params = {}, sessionId) => {
  const id = nextId++;
  ws.send(JSON.stringify({ id, method, params, sessionId }));
  return new Promise((resolve, reject) => waiting.set(id, { resolve, reject }));
};

let exit = 1;
try {
  const { targetId } = await cdp("Target.createTarget", { url: "about:blank" });
  const { sessionId } = await cdp("Target.attachToTarget", { targetId, flatten: true });
  await cdp("Runtime.enable", {}, sessionId);
  await cdp("Page.navigate", { url: `${url}/index.html` }, sessionId);
  const { result } = await cdp(
    "Runtime.evaluate",
    {
      expression: `(async () => {
        for (let i = 0; i < 600 && !window.__smoke; i++) await new Promise(r => setTimeout(r, 100));
        return window.__smoke ? await window.__smoke : { status: "fail", error: "no marker" };
      })()`,
      awaitPromise: true,
      returnByValue: true,
    },
    sessionId,
  );
  const r = result.value;
  console.log("vite app report:", JSON.stringify(r));
  if (r?.status === "pass" && r.rows === 1 && r.names?.[0] === "vite") {
    console.log("PASS: zero-config Vite build emitted worker+wasm and ran end to end");
    exit = 0;
  } else {
    console.error("FAIL: built Vite app did not produce the positive marker");
  }
} catch (err) {
  console.error("FAIL:", err.message ?? err);
} finally {
  ws.close();
  cleanup();
}
process.exit(exit);
