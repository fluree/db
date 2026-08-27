/**
 * `useConnectionState`: the client's connection lifecycle as React state.
 * SSR reports "connecting" — the live machinery is browser-only.
 */

import { useCallback, useSyncExternalStore } from "react";
import type { ConnectionState } from "../core/types.js";
import { useFlureeClient } from "./context.js";

function getServerSnapshot(): ConnectionState {
  return "connecting";
}

export function useConnectionState(): ConnectionState {
  const client = useFlureeClient();
  const subscribe = useCallback(
    (onChange: () => void) => client.onConnectionChange(onChange),
    [client],
  );
  const getSnapshot = useCallback(() => client.connectionState(), [client]);
  return useSyncExternalStore(subscribe, getSnapshot, getServerSnapshot);
}
