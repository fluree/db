/**
 * React Context here carries exactly ONE thing: the `LiveClient` instance.
 * Data never flows through context (every consumer would re-render on every
 * change); components get data through per-query subscriptions in
 * `useQuery`. The provider value is the stable client object, so the
 * provider itself never causes re-renders.
 */

import { createContext, useContext } from "react";
import type { ReactNode } from "react";
import type { LiveClient } from "../core/liveClient.js";

const FlureeContext = createContext<LiveClient | null>(null);

export interface FlureeProviderProps {
  client: LiveClient;
  children?: ReactNode;
}

export function FlureeProvider({ client, children }: FlureeProviderProps) {
  return (
    <FlureeContext.Provider value={client}>{children}</FlureeContext.Provider>
  );
}

export function useFlureeClient(): LiveClient {
  const client = useContext(FlureeContext);
  if (client === null) {
    throw new Error(
      "No FlureeProvider found. Wrap your component tree in " +
        "<FlureeProvider client={...}> (create the client once with " +
        "createClient and keep it stable).",
    );
  }
  return client;
}
