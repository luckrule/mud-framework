/**
 * Portal <-> Server IPC protocol (ADR-0002).
 *
 * This is a different protocol from the client wire format in
 * `runtime/codec.ts`: that one is JSON over a Transport, this one is an object
 * channel with structured serialization. The two must not share types or
 * validators, because a change to one must not silently change the other.
 */
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

/** Portal -> Server: deliver one Request (first delivery or replay). */
export interface RequestMessage {
  readonly type: 'request';
  /** Allocated by Portal and kept unchanged across replay (SPEC 7.3). */
  readonly requestId: string;
  readonly connectionId: string;
  readonly clientRequestId: string;
  /** Absent while the connection is not yet bound to a Session. */
  readonly sessionId?: string;
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

/** Portal -> Server: shut down in an orderly fashion. */
export interface ShutdownMessage {
  readonly type: 'shutdown';
}

/** Server -> Portal: readiness barrier, sent only after init and self-check. */
export interface ReadyMessage {
  readonly type: 'ready';
  readonly pid: number;
  readonly capabilities: StorageCapabilities;
}

export interface ResultError {
  readonly code: string;
  readonly message: string;
}

export interface ResultSuccessMessage {
  readonly type: 'result';
  readonly requestId: string;
  readonly ok: true;
  /** Handler return value; `unknown` admits `undefined` for a void handler. */
  readonly result: unknown;
}

export interface ResultFailureMessage {
  readonly type: 'result';
  readonly requestId: string;
  readonly ok: false;
  readonly error: ResultError;
}

/**
 * One message type discriminated by `ok`, deliberately not split into
 * `result`/`error` message types: Portal correlates both outcomes by the same
 * `requestId`, and a single type keeps the correlation switch in one place.
 */
export type ResultMessage = ResultSuccessMessage | ResultFailureMessage;

/** Server -> Portal: shutdown finished. */
export interface ClosedMessage {
  readonly type: 'closed';
  readonly reason: string;
}

export type IpcMessage =
  | HelloMessage
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
 * Unknown extra fields are tolerated so a newer peer can add fields without
 * breaking an older one; known fields are checked strictly.
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
    case 'request':
      return buildRequest(raw);
    case 'cancel':
      return {
        type: 'cancel',
        requestId: readString(raw, 'cancel', 'requestId', true),
        code: readString(raw, 'cancel', 'code', true),
      };
    case 'shutdown':
      return { type: 'shutdown' };
    case 'ready':
      return {
        type: 'ready',
        pid: readInteger(raw, 'ready', 'pid', 1),
        capabilities: readCapabilities(raw, 'ready', 'capabilities'),
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
  };
}

function buildResult(raw: Record<string, unknown>): ResultMessage {
  const requestId = readString(raw, 'result', 'requestId', true);
  if (!readBoolean(raw, 'result', 'ok')) {
    return {
      type: 'result',
      requestId,
      ok: false,
      error: readResultError(raw, 'result', 'error'),
    };
  }
  return { type: 'result', requestId, ok: true, result: raw.result };
}

function readResultError(
  raw: Record<string, unknown>,
  messageType: string,
  field: string,
): ResultError {
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

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function describeValue(value: unknown): string {
  if (value === null) {
    return 'null';
  }
  if (Array.isArray(value)) {
    return 'array';
  }
  if (typeof value === 'string') {
    return `string ${JSON.stringify(value)}`;
  }
  if (typeof value === 'number') {
    return `number ${value.toString()}`;
  }
  if (typeof value === 'boolean') {
    return `boolean ${value ? 'true' : 'false'}`;
  }
  return typeof value;
}

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
