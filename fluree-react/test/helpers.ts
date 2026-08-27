/**
 * Shared test doubles: a fully scriptable `LiveTransport` (for core and
 * adapter tests) and a recording `TransportSink` (for transport tests).
 */

import type {
  CycleUpdate,
  LiveTransport,
  SubscriptionSpec,
  TransportSink,
} from "../src/core/transport.js";
import type { ConnectionState, ResolvedSpec } from "../src/core/types.js";

export class FakeTransport implements LiveTransport {
  sink: TransportSink | undefined;
  readonly subscribes: SubscriptionSpec[] = [];
  readonly unsubscribes: number[] = [];
  readonly oneShots: ResolvedSpec[] = [];
  oneShotResult: unknown = { ok: true };
  private state: ConnectionState = "connecting";
  closed = false;

  start(sink: TransportSink): void {
    this.sink = sink;
  }

  subscribe(spec: SubscriptionSpec): void {
    this.subscribes.push(spec);
  }

  unsubscribe(subId: number): void {
    this.unsubscribes.push(subId);
  }

  async fetchOnce(spec: ResolvedSpec): Promise<unknown> {
    this.oneShots.push(spec);
    return this.oneShotResult;
  }

  connectionState(): ConnectionState {
    return this.state;
  }

  close(): void {
    this.closed = true;
  }

  emit(cycle: CycleUpdate): void {
    this.sink?.onCycle(cycle);
  }

  setConnection(state: ConnectionState): void {
    this.state = state;
    this.sink?.onConnection(state);
  }

  /** subId assigned to the nth transport subscription (creation order). */
  subId(n: number): number {
    const spec = this.subscribes[n];
    if (!spec) throw new Error(`no subscription #${n}`);
    return spec.subId;
  }
}

export interface RecordingSink extends TransportSink {
  cycles: CycleUpdate[];
  states: ConnectionState[];
}

export function recordingSink(): RecordingSink {
  const sink: RecordingSink = {
    cycles: [],
    states: [],
    onCycle(cycle) {
      sink.cycles.push(cycle);
    },
    onConnection(state) {
      sink.states.push(state);
    },
  };
  return sink;
}
