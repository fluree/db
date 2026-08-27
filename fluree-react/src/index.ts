// Client
export { createClient } from "./createClient.js";
export type {
  ClientConfig,
  PeerClientConfig,
  RemoteClientConfig,
  TransportClientConfig,
} from "./createClient.js";
export { LiveClient } from "./core/liveClient.js";
export type { ClientOptions } from "./core/liveClient.js";

// React adapter
export { FlureeProvider, useFlureeClient } from "./react/context.js";
export type { FlureeProviderProps } from "./react/context.js";
export { useQuery } from "./react/useQuery.js";
export { useConnectionState } from "./react/useConnectionState.js";

// Core types
export type {
  ConnectionState,
  QueryError,
  QueryFormat,
  QueryKind,
  QueryOptions,
  QueryResult,
  QueryStatus,
  ResolvedSpec,
} from "./core/types.js";

// Transport seam (implemented by remote mode today; the wasm browser-peer
// transport implements the same contract)
export type {
  CycleChange,
  CycleErrored,
  CycleUpdate,
  LiveTransport,
  SubscriptionSpec,
  TransportSink,
} from "./core/transport.js";
export { RemoteTransport } from "./remote/remoteTransport.js";
export type { RemoteTransportOptions } from "./remote/remoteTransport.js";
export { PeerTransport } from "./peer/peerTransport.js";
export type { PeerTransportOptions } from "./peer/peerTransport.js";
export type {
  PeerConnect,
  PeerConnectOptions,
  PeerCycle,
  PeerEngine,
  PeerEngineState,
  PeerError,
  PeerLedger,
  PeerSubscription,
} from "./peer/peerEngine.js";

// Utilities
export { replaceEqualDeep } from "./core/structuralShare.js";
