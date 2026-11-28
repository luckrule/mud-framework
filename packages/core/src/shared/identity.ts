import type { LogFields } from '../logging/types.ts';

/**
 * The ids every request carries from entry to terminal state (SPEC 7.3).
 *
 * Shared by the in-process shapes and the wire format because these four fields
 * mean the same thing everywhere: `requestId` is minted by the framework on
 * entry and survives replay, `connectionId` names the transport link,
 * `clientRequestId` is the client's own correlation key, and `sessionId` is
 * present only when the connection is already bound to a Session — a handler
 * that starts one declares it through `bindSession`, which takes effect only if
 * the request succeeds (SPEC 2).
 *
 * `clientRequestId` is present only when the request carries one: a request
 * that entered through the client wire format always does (`Envelope.id` is
 * required non-empty), while a direct `dispatch` or an IPC message may omit it.
 * An empty string is not a way to say "none" (SPEC 7.3). `connectionId` is an
 * empty string rather than absent when a direct `dispatch` has no connection,
 * and `sessionId` uses absence to mean "not bound".
 *
 * `readonly` because two of the five users are already read-only shapes and a
 * field nobody assigns needs no mutable declaration. It is also the whole safety
 * argument for sharing this with the wire format: strings serialize, handles do
 * not, so nothing beyond these ids may move in here.
 */
export interface RequestIdentity {
  readonly requestId: string;
  readonly connectionId: string;
  readonly clientRequestId?: string;
  readonly sessionId?: string;
}

/**
 * `RequestIdentity` as request-level log bindings (SPEC 7.1). Both processes
 * build these fields, so the mapping lives here rather than in either one.
 *
 * `clientRequestId` and `sessionId` are omitted, not written as `undefined` or
 * an empty string, when absent: a request that carries no correlation key and a
 * connection that holds no Session both mean "not there" (SPEC 2, 7.3).
 */
export function requestLogFields(identity: RequestIdentity): LogFields {
  const fields: LogFields = {
    requestId: identity.requestId,
    connectionId: identity.connectionId,
  };
  if (identity.clientRequestId !== undefined) {
    fields.clientRequestId = identity.clientRequestId;
  }
  if (identity.sessionId !== undefined) {
    fields.sessionId = identity.sessionId;
  }
  return fields;
}
