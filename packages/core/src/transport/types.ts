export interface TransportCapabilities {
  /** False means only text frames travel; binary payloads must be encoded. */
  binaryFrames: boolean;
  fragmentation: boolean;
  /** Per-message ceiling; Number.POSITIVE_INFINITY when unbounded. */
  maxMessageBytes: number;
  /** Whether the transport can push back on the writer instead of dropping. */
  backpressure: boolean;
  orderedDelivery: boolean;
  heartbeat: 'protocol' | 'application' | 'none';
}

export interface TransportConnection {
  readonly id: string;
  /**
   * A promise, not a fire-and-forget call: the transport may have to wait for
   * the underlying write or for backpressure to clear, and a failed write must
   * reach the caller as a rejection instead of disappearing into the stack.
   */
  send(payload: string | Uint8Array): Promise<void>;
  close(code?: number, reason?: string): Promise<void>;
}

export interface TransportHandlers {
  onConnect(connection: TransportConnection): void | Promise<void>;
  /**
   * May return a promise so the transport can await delivery: ordered
   * processing and backpressure both need the handler's completion, and the
   * execution model is async throughout (SPEC 3).
   */
  onMessage(
    connection: TransportConnection,
    payload: string | Uint8Array,
  ): void | Promise<void>;
  onDisconnect(
    connection: TransportConnection,
    reason: string,
  ): void | Promise<void>;
}

/**
 * The four things a transport owns: accept, deliver inbound, write outbound,
 * close. Session routing, input buffering, replay and authentication belong to
 * the connection process and MUST NOT be pushed down here (SPEC 5.4).
 */
export interface Transport {
  capabilities(): TransportCapabilities;
  start(handlers: TransportHandlers): Promise<void>;
  stop(): Promise<void>;
}
