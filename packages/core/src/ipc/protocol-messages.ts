/**
 * Portal <-> Server IPC protocol (ADR-0002).
 *
 * This is a different protocol from the client wire format in
 * `shared/codec.ts`: that one is JSON over a Transport, this one is an object
 * channel with structured serialization. The two must not share types or
 * validators, because a change to one must not silently change the other.
 */
import type { ServerEventTarget } from '../shared/events.ts';
import type { ExecutorOutcome } from '../shared/executor.ts';
import type { RequestIdentity } from '../shared/identity.ts';
import type { StorageCapabilities } from '../storage/types.ts';

/** Portal -> Server: handshake. */
export interface HelloMessage {
  readonly type: 'hello';
}

/**
 * Server -> Portal: the handshake was accepted and the Server is waiting for
 * activation (ADR-0007).
 *
 * Nothing that can write storage exists yet: no backend, no Extension, no
 * handler. `pid` names the process so a Portal can observe the restart.
 */
export interface PreparedMessage {
  readonly type: 'prepared';
  readonly pid: number;
}

/**
 * Portal -> Server: open storage and become ready (ADR-0007).
 *
 * Sent only after the previous Server process is known to be gone, so the
 * storage never has two writers (INV-002). The Server answers with `ready`.
 */
export interface ActivateMessage {
  readonly type: 'activate';
}

/** Portal -> Server: deliver one Request (first delivery or replay). */
export interface RequestMessage extends RequestIdentity {
  readonly type: 'request';
  readonly method: string;
  /**
   * Opaque to the framework. `unknown` already admits `undefined`, so a
   * handler that takes no argument needs no separate message shape.
   */
  readonly payload: unknown;
  readonly timeoutMs: number;
  /** 0 on first delivery; incremented by Portal on each replay (SPEC 7.3). */
  readonly replayCount: number;
}

/**
 * Portal -> Server: the Request was disconnected or timed out, stop work.
 * Carries no acknowledgement of its own: only `result` confirms a Request
 * (SPEC §2), so Portal drops its own bookkeeping when it sends this.
 */
export interface CancelMessage {
  readonly type: 'cancel';
  readonly requestId: string;
  /** Framework error code, e.g. `FRAMEWORK_TIMEOUT` (SPEC 7.2). */
  readonly code: string;
}

/**
 * Portal -> Server: shut down in an orderly fashion.
 *
 * `timeoutMs` is the budget the Server has for the whole graceful close (drain
 * plus backend close), declared by the Portal that is waiting for it. It is a
 * one-way derivation: the Portal knows how long it will wait, so the Server must
 * fit inside that rather than derive its own budget from its request ceiling —
 * the two used to be unrelated, and a Server closing inside its own budget was
 * killed by a Portal that had already given up.
 *
 * Required, and validated: the Server's whole graceful close is bounded by it,
 * so a message without it cannot be acted on.
 */
export interface ShutdownMessage {
  readonly type: 'shutdown';
  readonly timeoutMs: number;
}

/** Server -> Portal: readiness barrier, sent only after init and self-check. */
export interface ReadyMessage {
  readonly type: 'ready';
  readonly pid: number;
  readonly capabilities: StorageCapabilities;
}

/**
 * One message type discriminated by `ok`, deliberately not split into
 * `result`/`error` message types: Portal correlates both outcomes by the same
 * `requestId`, and a single type keeps the correlation switch in one place.
 *
 * The outcome half is `ExecutorOutcome` itself rather than a copy of its
 * fields: the two sides are the same value, and a second declaration only
 * creates a second place to forget a field. Only the envelope (`type`,
 * `requestId`) is added here.
 */
export type ResultMessage = {
  readonly type: 'result';
  readonly requestId: string;
  /**
   * The Session the request carried on entry, echoed back by the Server so a
   * result that arrives after the Portal settled the request still names it.
   *
   * Deliberately separate from the `sessionChange` that `ExecutorOutcome`
   * carries: that one is a change the handler declared during this request and
   * only a success can have it. A failure carries this echo and no
   * `sessionChange`. Omitted when the request entered with no Session.
   */
  readonly requestSessionId?: string;
} & ExecutorOutcome;

/** Server -> Portal: shutdown finished. */
export interface ClosedMessage {
  readonly type: 'closed';
  readonly reason: string;
}

/**
 * Server -> Portal: one event a committed request emitted (ADR-0008).
 *
 * A separate message rather than a field on `result` because the two have
 * different delivery rules: a result is owed to a waiting caller, an event is
 * best-effort and may be dropped at either process without affecting the
 * request. `eventId` is `${requestId}:${index}`; a replay that emits in the
 * same order produces the same ids, so clients can dedupe.
 */
export interface EventMessage {
  readonly type: 'event';
  readonly eventId: string;
  readonly target: ServerEventTarget;
  readonly event: string;
  readonly payload: unknown;
}

export type IpcMessage =
  | HelloMessage
  | PreparedMessage
  | ActivateMessage
  | RequestMessage
  | CancelMessage
  | ShutdownMessage
  | ReadyMessage
  | ResultMessage
  | EventMessage
  | ClosedMessage;

export type IpcParseResult =
  | { readonly ok: true; readonly message: IpcMessage }
  | { readonly ok: false; readonly reason: string };

export const KNOWN_TYPES = [
  'hello',
  'prepared',
  'activate',
  'request',
  'cancel',
  'shutdown',
  'ready',
  'result',
  'event',
  'closed',
] as const;
