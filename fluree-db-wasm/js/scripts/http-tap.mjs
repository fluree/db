// A recording reverse proxy in front of the Fluree server.
//
// The browser peer talks to this; every request it makes is recorded with its
// method, path, and upstream status before being streamed through unchanged.
// That turns "the query returned rows" into "the query returned rows AND the
// browser pulled N CID-addressed CAS objects over `GET /storage/objects/{cid}`
// and held an SSE stream open" — evidence, not inference.
//
// It is a byte-for-byte pipe (`node:http` streams, never buffered), so SSE
// flows through live and the server's own CORS/ETag headers reach the page
// exactly as sent.

import { readFile, stat } from "node:fs/promises";
import { createServer, request as httpRequest } from "node:http";
import { extname, join, normalize, resolve, sep } from "node:path";

const MIME = {
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".mjs": "text/javascript; charset=utf-8",
  ".map": "application/json; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".wasm": "application/wasm",
  ".css": "text/css; charset=utf-8",
};

/**
 * Start the tap in front of `upstreamUrl` (e.g. `http://127.0.0.1:8090`).
 * Resolves `{ url, apiBase, requests, matching, close }` where `requests` is
 * the live log array.
 *
 * Options:
 * - `staticRoot` — also serve files from this directory for any path outside
 *   `/v1/fluree/`. That puts the page and the API on ONE origin, which makes
 *   every request same-origin and so removes CORS preflights entirely — the
 *   control arm for measuring what preflights cost.
 * - `delayMs` — sleep this long before forwarding each proxied request,
 *   standing in for network latency. Loopback RTT is ~0.1 ms, which hides the
 *   per-round-trip cost that dominates on a real link.
 */
export function startTap(upstreamUrl, { staticRoot, delayMs = 0 } = {}) {
  const upstream = new URL(upstreamUrl);
  const requests = [];
  const staticBase = staticRoot ? resolve(staticRoot) : null;

  const server = createServer(async (req, res) => {
    if (staticBase && !req.url.startsWith("/v1/fluree/")) {
      await serveStatic(staticBase, req, res);
      return;
    }
    const entry = {
      method: req.method,
      path: req.url,
      status: null,
      startedAt: performance.now(),
      endedAt: null,
      bytes: 0,
    };
    requests.push(entry);

    if (delayMs > 0) await new Promise((r) => setTimeout(r, delayMs));

    const proxied = httpRequest(
      {
        hostname: upstream.hostname,
        port: upstream.port,
        path: req.url,
        method: req.method,
        headers: { ...req.headers, host: upstream.host },
      },
      (upRes) => {
        entry.status = upRes.statusCode;
        res.writeHead(upRes.statusCode, upRes.headers);
        upRes.on("data", (chunk) => { entry.bytes += chunk.length; });
        upRes.on("end", () => { entry.endedAt = performance.now(); });
        upRes.pipe(res);
      },
    );
    proxied.on("error", (err) => {
      entry.status = 0;
      entry.error = String(err.message ?? err);
      if (!res.headersSent) res.writeHead(502, { "content-type": "text/plain" });
      res.end(`tap upstream error: ${err.message}`);
    });
    // A client that goes away mid-stream (SSE on page close) must tear down
    // the upstream leg too, or the server keeps the subscription alive.
    res.on("close", () => proxied.destroy());
    req.pipe(proxied);
  });

  return new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const { port } = server.address();
      resolve({
        url: `http://127.0.0.1:${port}`,
        apiBase: `http://127.0.0.1:${port}/v1/fluree`,
        requests,
        /**
         * Requests whose path starts with `prefix`, filtered by method.
         *
         * The method is required, not optional: a cross-origin `GET` is
         * preceded by an `OPTIONS` preflight on the same path, so a
         * path-only filter double-counts every request and would report
         * twice the CAS fetches that actually happened.
         */
        matching: (method, prefix) =>
          requests.filter((r) => r.method === method && r.path.startsWith(prefix)),
        close: () => server.close(),
      });
    });
  });
}

/** Serve one file from `base`, or 404. Same MIME table as serve.mjs. */
async function serveStatic(base, req, res) {
  try {
    const urlPath = decodeURIComponent(new URL(req.url, "http://x").pathname);
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
}
