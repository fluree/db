/**
 * Client construction. Two modes behind the same hooks API:
 *
 * - remote mode (pass `url`): queries over the server HTTP API, re-run when
 *   the server's SSE endpoint announces new commits. No wasm; SSR-friendly;
 *   works against any Fluree server with query + events.
 * - custom transport (pass `transport`): the seam the wasm browser-peer
 *   transport plugs into — same `LiveTransport` contract, same hooks.
 */

import { LiveClient } from "./core/liveClient.js";
import type { LiveTransport } from "./core/transport.js";
import { RemoteTransport } from "./remote/remoteTransport.js";
import type { RemoteTransportOptions } from "./remote/remoteTransport.js";

export interface RemoteClientConfig extends RemoteTransportOptions {
  gcTime?: number;
}

export interface TransportClientConfig {
  transport: LiveTransport;
  gcTime?: number;
}

export type ClientConfig = RemoteClientConfig | TransportClientConfig;

export function createClient(config: ClientConfig): LiveClient {
  const transport =
    "transport" in config ? config.transport : new RemoteTransport(config);
  const options = config.gcTime !== undefined ? { gcTime: config.gcTime } : {};
  return new LiveClient(transport, options);
}
