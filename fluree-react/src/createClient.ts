/**
 * Client construction. Three ways to build the same client, all behind one
 * hooks API:
 *
 * - remote mode (pass `url`): queries over the server HTTP API, re-run when
 *   the server's SSE endpoint announces new commits. No wasm; SSR-friendly;
 *   works against any Fluree server with query + events.
 * - peer mode (pass `peer`): the wasm engine in a web worker hears the same
 *   commits, applies them locally, and re-runs the affected queries at
 *   memory speed. `peer.connect` is `@fluree/db-wasm`'s `connect`, passed in
 *   so this package keeps no dependency on the wasm engine.
 * - custom transport (pass `transport`): the raw `LiveTransport` seam.
 */

import { LiveClient } from "./core/liveClient.js";
import type { LiveTransport } from "./core/transport.js";
import { PeerTransport } from "./peer/peerTransport.js";
import type { PeerTransportOptions } from "./peer/peerTransport.js";
import { RemoteTransport } from "./remote/remoteTransport.js";
import type { RemoteTransportOptions } from "./remote/remoteTransport.js";

export interface RemoteClientConfig extends RemoteTransportOptions {
  gcTime?: number;
}

export interface PeerClientConfig {
  peer: PeerTransportOptions;
  gcTime?: number;
}

export interface TransportClientConfig {
  transport: LiveTransport;
  gcTime?: number;
}

export type ClientConfig =
  | RemoteClientConfig
  | PeerClientConfig
  | TransportClientConfig;

function transportFor(config: ClientConfig): LiveTransport {
  if ("transport" in config) return config.transport;
  if ("peer" in config) return new PeerTransport(config.peer);
  return new RemoteTransport(config);
}

export function createClient(config: ClientConfig): LiveClient {
  const options = config.gcTime !== undefined ? { gcTime: config.gcTime } : {};
  return new LiveClient(transportFor(config), options);
}
