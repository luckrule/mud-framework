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
 * `connectionId` and `clientRequestId` are empty strings rather than absent when
 * the caller supplied none: a direct `dispatch` has no connection. Only
 * `sessionId` uses absence to mean something ("not bound").
 *
 * `readonly` because two of the five users are already read-only shapes and a
 * field nobody assigns needs no mutable declaration. It is also the whole safety
 * argument for sharing this with the wire format: strings serialize, handles do
 * not, so nothing beyond these ids may move in here.
 */
export interface RequestIdentity {
  readonly requestId: string;
  readonly connectionId: string;
  readonly clientRequestId: string;
  readonly sessionId?: string;
}
