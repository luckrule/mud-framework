import type { ExecutorOutcome, SessionChange } from '../shared/executor.ts';
/**
 * Portal <-> Server IPC protocol (ADR-0002).
 *
 * This is a different protocol from the client wire format in
 * `client/codec.ts`: that one is JSON over a Transport, this one is an object
 * channel with structured serialization. The two must not share types or
 * validators, because a change to one must not silently change the other.
 */
import { describeValue, isRecord, messageOf } from '../shared/guards.ts';
import type { RequestIdentity } from '../shared/identity.ts';
import {
  type CapabilityCheck,
  checkWireCapabilities,
} from '../storage/capabilities.ts';
import type { StorageCapabilities } from '../storage/types.ts';

/** Portal -> Server: handshake. */
export interface HelloMessage {
  readonly type: 'hello';
  /**
   * Incremented on every Portal start so Server can tell a new Portal instance
   * from the one it was already talking to.
   */
  readonly portalGeneration: number;
}

/**
 * Server -> Portal: the handshake was accepted and the Server is waiting for
 * activation (ADR-0007).
 *
 * Nothing that can write storage exists yet: no backend, no Extension, no
 * handler. `pid` names the process so a Portal can observe the restart, and
 * `codeVersion` is the same opaque echo `ready` carries.
 */
export interface PreparedMessage {
  readonly type: 'prepared';
  readonly pid: number;
  readonly codeVersion?: string;
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
  /**
   * The application's opaque code version at the moment this Request was
   * accepted. A replay keeps the value its pending entry was stamped with, so
   * it names the code the original delivery ran under, not the replacement
   * Server's. The framework only reports a difference (SPEC 6.4).
   */
  readonly codeVersion?: string;
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
  /**
   * The application's opaque code version this Server is running. Omitted when
   * the deployment declared none, which is not the same as reporting a
   * different version (SPEC 6.4).
   */
  readonly codeVersion?: string;
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

export type IpcMessage =
  | HelloMessage
  | PreparedMessage
  | ActivateMessage
  | RequestMessage
  | CancelMessage
  | ShutdownMessage
  | ReadyMessage
  | ResultMessage
  | ClosedMessage;

export type IpcParseResult =
  | { readonly ok: true; readonly message: IpcMessage }
  | { readonly ok: false; readonly reason: string };

const KNOWN_TYPES = [
  'hello',
  'prepared',
  'activate',
  'request',
  'cancel',
  'shutdown',
  'ready',
  'result',
  'closed',
] as const;

/**
 * Narrows `unknown` (whatever the peer put on the channel) to a known message.
 *
 * Never throws: the IPC callback's only error channel is its return value, and
 * an exception there is logged and dropped by the runtime. Dropping it would
 * turn a malformed message into "the request never gets a result", which is
 * indistinguishable from a hung Server. A `reason` string is always produced so
 * the caller can log a diagnosable field (SPEC 7.1).
 *
 * Extra fields are not part of the parsed result: the parser reads the known
 * fields and checks them strictly, so anything else stays out of the message
 * the caller receives.
 */
export function parseIpcMessage(raw: unknown): IpcParseResult {
  try {
    return { ok: true, message: buildMessage(raw) };
  } catch (error) {
    if (error instanceof ShapeError) {
      return { ok: false, reason: error.message };
    }
    // Validation only throws ShapeError; anything else is a bug in this file,
    // still reported as a rejection so the caller has one failure path.
    return {
      ok: false,
      reason: `ipc message validation failed unexpectedly: ${messageOf(error)}`,
    };
  }
}

function buildMessage(raw: unknown): IpcMessage {
  if (!isRecord(raw)) {
    throw new ShapeError(
      `ipc message must be an object, received ${describeValue(raw)}`,
    );
  }
  const type = raw.type;
  if (typeof type !== 'string') {
    throw new ShapeError(
      `ipc message "type" must be a string, received ${describeValue(type)}`,
    );
  }
  switch (type) {
    case 'hello':
      return {
        type: 'hello',
        portalGeneration: readInteger(raw, 'hello', 'portalGeneration', 0),
      };
    case 'prepared':
      return {
        type: 'prepared',
        pid: readInteger(raw, 'prepared', 'pid', 1),
        codeVersion: readOptionalString(raw, 'prepared', 'codeVersion'),
      };
    case 'activate':
      return { type: 'activate' };
    case 'request':
      return buildRequest(raw);
    case 'cancel':
      return {
        type: 'cancel',
        requestId: readString(raw, 'cancel', 'requestId', true),
        code: readString(raw, 'cancel', 'code', true),
      };
    case 'shutdown':
      return {
        type: 'shutdown',
        timeoutMs: readPositiveNumber(raw, 'shutdown', 'timeoutMs'),
      };
    case 'ready':
      return {
        type: 'ready',
        pid: readInteger(raw, 'ready', 'pid', 1),
        capabilities: readCapabilities(raw, 'ready', 'capabilities'),
        codeVersion: readOptionalString(raw, 'ready', 'codeVersion'),
      };
    case 'result':
      return buildResult(raw);
    case 'closed':
      return {
        type: 'closed',
        reason: readString(raw, 'closed', 'reason', false),
      };
    default:
      throw new ShapeError(
        `unknown ipc message type ${JSON.stringify(type)}; expected one of ${KNOWN_TYPES.join(', ')}`,
      );
  }
}

function buildRequest(raw: Record<string, unknown>): RequestMessage {
  return {
    type: 'request',
    requestId: readString(raw, 'request', 'requestId', true),
    connectionId: readString(raw, 'request', 'connectionId', true),
    clientRequestId: readString(raw, 'request', 'clientRequestId', true),
    sessionId: readOptionalString(raw, 'request', 'sessionId'),
    method: readString(raw, 'request', 'method', true),
    payload: raw.payload,
    timeoutMs: readPositiveNumber(raw, 'request', 'timeoutMs'),
    replayCount: readInteger(raw, 'request', 'replayCount', 0),
    codeVersion: readOptionalString(raw, 'request', 'codeVersion'),
  };
}

function buildResult(raw: Record<string, unknown>): ResultMessage {
  const requestId = readString(raw, 'result', 'requestId', true);
  // Read for both outcomes: a failed result has no `sessionChange` of its own,
  // so the echo is the only Session it can name.
  const requestSessionId = readOptionalString(
    raw,
    'result',
    'requestSessionId',
  );
  if (!readBoolean(raw, 'result', 'ok')) {
    return {
      type: 'result',
      requestId,
      requestSessionId,
      ok: false,
      error: readResultError(raw, 'result', 'error'),
    };
  }
  return {
    type: 'result',
    requestId,
    requestSessionId,
    ok: true,
    result: raw.result,
    // Absent means "no Session change" (SPEC 2), so it is passed through as
    // `undefined` rather than defaulted to an empty object.
    sessionChange: readSessionChange(raw, 'result', 'sessionChange'),
  };
}

/**
 * Strict by construction: a malformed change must be refused here, because a
 * permissive parse would let a bad `sessionId` or an unknown change type reach
 * the Portal's routing table (SPEC 3.7).
 */
function readSessionChange(
  raw: Record<string, unknown>,
  messageType: string,
  field: string,
): SessionChange | undefined {
  if (raw[field] === undefined) {
    return undefined;
  }
  const record = readRecord(raw, messageType, field);
  const prefix = `${messageType}.${field}`;
  const type = record.type;
  if (type === 'bind') {
    return {
      type: 'bind',
      sessionId: readString(record, prefix, 'sessionId', true),
    };
  }
  if (type === 'unbind') {
    if (record.sessionId !== undefined) {
      throw new ShapeError(
        fieldReason(
          prefix,
          'sessionId',
          'absent for an unbind',
          record.sessionId,
        ),
      );
    }
    return { type: 'unbind' };
  }
  throw new ShapeError(fieldReason(prefix, 'type', '"bind" or "unbind"', type));
}

function readResultError(
  raw: Record<string, unknown>,
  messageType: string,
  field: string,
): { code: string; message: string } {
  const record = readRecord(raw, messageType, field);
  const prefix = `${messageType}.${field}`;
  return {
    code: readString(record, prefix, 'code', true),
    message: readString(record, prefix, 'message', false),
  };
}

function readCapabilities(
  raw: Record<string, unknown>,
  messageType: string,
  field: string,
): StorageCapabilities {
  const path = `${messageType}.${field}`;
  let checked: CapabilityCheck;
  try {
    checked = checkWireCapabilities(raw[field], path);
  } catch (error) {
    // The shared validator reports its own rejections as `ok: false`; anything
    // it throws comes from the input, so it is a shape problem too.
    throw new ShapeError(
      `capability "${path}" could not be validated: ${messageOf(error)}`,
    );
  }
  if (!checked.ok) {
    throw new ShapeError(checked.reason);
  }
  return checked.capabilities;
}

/**
 * Internal control flow for the field readers below. Keeping it an Error lets
 * the readers stay flat; `parseIpcMessage` is the only place it is observed.
 */
class ShapeError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'ShapeError';
  }
}

function readRecord(
  raw: Record<string, unknown>,
  messageType: string,
  field: string,
): Record<string, unknown> {
  const value = raw[field];
  if (!isRecord(value)) {
    throw new ShapeError(fieldReason(messageType, field, 'an object', value));
  }
  return value;
}

function readString(
  raw: Record<string, unknown>,
  messageType: string,
  field: string,
  nonEmpty: boolean,
): string {
  const value = raw[field];
  if (typeof value !== 'string') {
    throw new ShapeError(fieldReason(messageType, field, 'a string', value));
  }
  if (nonEmpty && value.length === 0) {
    throw new ShapeError(
      fieldReason(messageType, field, 'a non-empty string', value),
    );
  }
  return value;
}

function readOptionalString(
  raw: Record<string, unknown>,
  messageType: string,
  field: string,
): string | undefined {
  return raw[field] === undefined
    ? undefined
    : readString(raw, messageType, field, true);
}

function readBoolean(
  raw: Record<string, unknown>,
  messageType: string,
  field: string,
): boolean {
  const value = raw[field];
  if (typeof value !== 'boolean') {
    throw new ShapeError(fieldReason(messageType, field, 'a boolean', value));
  }
  return value;
}

function readPositiveNumber(
  raw: Record<string, unknown>,
  messageType: string,
  field: string,
): number {
  const value = raw[field];
  if (typeof value !== 'number' || !Number.isFinite(value) || value <= 0) {
    throw new ShapeError(
      fieldReason(messageType, field, 'a finite number greater than 0', value),
    );
  }
  return value;
}

function readInteger(
  raw: Record<string, unknown>,
  messageType: string,
  field: string,
  minimum: number,
): number {
  const value = raw[field];
  if (
    typeof value !== 'number' ||
    !Number.isInteger(value) ||
    value < minimum
  ) {
    throw new ShapeError(
      fieldReason(messageType, field, `an integer >= ${minimum}`, value),
    );
  }
  return value;
}

function fieldReason(
  messageType: string,
  field: string,
  expected: string,
  value: unknown,
): string {
  return `ipc "${messageType}" field "${field}" must be ${expected}, received ${describeValue(value)}`;
}
