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

import { createServer, request as httpRequest } from "node:http";

/**
 * Start the tap in front of `upstreamUrl` (e.g. `http://127.0.0.1:8090`).
 * Resolves `{ url, requests, count, close }` where `requests` is the live
 * log array.
 */
export function startTap(upstreamUrl) {
  const upstream = new URL(upstreamUrl);
  const requests = [];

  const server = createServer((req, res) => {
    const entry = { method: req.method, path: req.url, status: null };
    requests.push(entry);

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
