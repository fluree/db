#!/usr/bin/env node
// Minimal static file server for the package directory (dist/, pkg/, demo/).
//
//   node scripts/serve.mjs [--port N]      # then open http://127.0.0.1:N/demo/
//
// Also imported by smoke-browser.mjs. Sends the MIME types the browser needs
// for `WebAssembly.instantiateStreaming` (application/wasm) and module
// workers (text/javascript). Deliberately sets NO cross-origin-isolation
// headers: the package must work without COOP/COEP, and this server is the
// proof.

import { createServer } from "node:http";
import { readFile, stat } from "node:fs/promises";
import { dirname, extname, join, normalize, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";

const MIME = {
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".mjs": "text/javascript; charset=utf-8",
  ".map": "application/json; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".wasm": "application/wasm",
  ".css": "text/css; charset=utf-8",
  ".ts": "text/plain; charset=utf-8",
};

export const packageRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");

/** Start serving `root`; resolves to { server, port, url }. Port 0 = ephemeral. */
export function startServer(root = packageRoot, port = 0) {
  const base = resolve(root);
  const server = createServer(async (req, res) => {
    try {
      const urlPath = decodeURIComponent(new URL(req.url ?? "/", "http://x").pathname);
      let file = normalize(join(base, urlPath));
      if (!file.startsWith(base + sep) && file !== base) {
        res.writeHead(403).end();
        return;
      }
      let st = await stat(file).catch(() => null);
      if (st?.isDirectory()) {
        file = join(file, "index.html");
        st = await stat(file).catch(() => null);
      }
      if (!st?.isFile()) {
        res.writeHead(404, { "content-type": "text/plain" }).end(`not found: ${urlPath}`);
        return;
      }
      const body = await readFile(file);
      res.writeHead(200, {
        "content-type": MIME[extname(file)] ?? "application/octet-stream",
        "content-length": body.length,
        "cache-control": "no-store",
      });
      res.end(body);
    } catch (err) {
      res.writeHead(500, { "content-type": "text/plain" }).end(String(err));
    }
  });
  return new Promise((resolveStart, reject) => {
    server.once("error", reject);
    server.listen(port, "127.0.0.1", () => {
      const actual = server.address().port;
      resolveStart({ server, port: actual, url: `http://127.0.0.1:${actual}` });
    });
  });
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const idx = process.argv.indexOf("--port");
  const port = idx > 0 ? Number(process.argv[idx + 1]) : 8787;
  const { url } = await startServer(packageRoot, port);
  console.log(`serving ${packageRoot}\n  demo:  ${url}/demo/\n  smoke: ${url}/demo/smoke.html`);
}
