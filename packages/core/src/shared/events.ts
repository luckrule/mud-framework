/**
 * Server-initiated events (ADR-0008). A handler buffers these during a
 * request; the framework sends them only after the request commits, so an
 * event never announces a state that rolled back.
 *
 * `ServerEventTarget` is the handler's address, not a connection: `session`
 * is resolved to the current holder by the Portal at delivery time, so an
 * event addressed to a Session follows a reconnect instead of pinning the
 * connection that was current when the handler ran.
 */
export type ServerEventTarget =
  | { readonly kind: 'connection'; readonly connectionId: string }
  | { readonly kind: 'session'; readonly sessionId: string };

/** What a handler buffers: no id yet, because only the sender names it. */
export interface BufferedServerEvent {
  readonly target: ServerEventTarget;
  readonly event: string;
  readonly payload: unknown;
}

/**
 * One event on its way out. `eventId` is `${requestId}:${index}`: stable for a
 * replay with the same emission order, so a client can dedupe an event it
 * received twice without the framework keeping delivery state.
 */
export interface ServerEvent extends BufferedServerEvent {
  readonly eventId: string;
}
