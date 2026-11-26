export interface TransportConnection {
  /**
   * MUST be unique for the life of the ConnectionLayer, and MUST NOT be reused
   * by a later connection: the layer keys its active-connection table, Session
   * routing, request-entry lookup and event delivery by this id. Reusing an id
   * after a disconnect would let a late inbound message land on the replacement
   * connection, and events addressed to the old connection be misrouted to it.
   * The default WS transport mints a UUID, which satisfies this.
   */
  readonly id: string;
  /**
   * A promise, not a fire-and-forget call: the transport may have to wait for
   * the underlying write or for backpressure to clear, and a failed write must
   * reach the caller as a rejection instead of disappearing into the stack.
   */
  send(payload: string | Uint8Array): Promise<void>;
  close(code?: number, reason?: string): Promise<void>;
}

/**
 * Contract: every callback for one connection MUST receive the same
 * `TransportConnection` object. The layer tells a stale disconnect from the
 * current connection by object identity, not by id, so a transport that hands
 * each callback a fresh wrapper would make a live connection look replaced and
 * let a late disconnect pass for the current one.
 */
export interface TransportHandlers {
  onConnect(connection: TransportConnection): void | Promise<void>;
  /**
   * May return a promise so the transport can await delivery: ordered
   * processing needs the handler's completion, and the execution model is
   * async throughout (SPEC 3).
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
 * the connection process and MUST NOT be pushed down here (SPEC 5.2).
 */
export interface Transport {
  /**
   * Optional: the transport's own upper bound on one frame write. A transport
   * that bounds its sends MAY expose it so the framework can bound a Server
   * event write at least as loosely. That keeps the Session's serial slot held
   * until the transport itself fails, instead of releasing it while a frame is
   * still being written and letting the next event overtake it (SPEC 3.8).
   *
   * Absent means "no declaration"; the layer then uses its own configuration.
   */
  readonly sendTimeoutMs?: number;
  start(handlers: TransportHandlers): Promise<void>;
  stop(): Promise<void>;
}
