import { isValidClientRequestId } from '../shared/client-request-id.ts';
import type { ServerEventTarget } from '../shared/events.ts';
import type { SessionChange } from '../shared/executor.ts';
import { describeValue, isRecord, messageOf } from '../shared/guards.ts';
import { checkWireCapabilities } from '../storage/capabilities.ts';
import type { StorageCapabilities } from '../storage/types.ts';
import type {
  EventMessage,
  IpcMessage,
  IpcParseResult,
  RequestMessage,
  ResultMessage,
} from './protocol-messages.ts';
import { KNOWN_TYPES } from './protocol-messages.ts';

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
      return { type: 'hello' };
    case 'prepared':
      return {
        type: 'prepared',
        pid: readInteger(raw, 'prepared', 'pid', 1),
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
      };
    case 'result':
      return buildResult(raw);
    case 'event':
      return buildEvent(raw);
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
    // Optional correlation key (SPEC 7.3): absent when the client supplied
    // none, and still rejected when present but empty.
    clientRequestId: readClientRequestId(raw, 'request', 'clientRequestId'),
    sessionId: readOptionalString(raw, 'request', 'sessionId'),
    method: readString(raw, 'request', 'method', true),
    payload: raw.payload,
    timeoutMs: readPositiveNumber(raw, 'request', 'timeoutMs'),
    replayCount: readInteger(raw, 'request', 'replayCount', 0),
  };
}

function buildEvent(raw: Record<string, unknown>): EventMessage {
  return {
    type: 'event',
    eventId: readString(raw, 'event', 'eventId', true),
    target: readEventTarget(raw, 'event', 'target'),
    event: readString(raw, 'event', 'event', true),
    // Opaque to the framework: any value the business put there is passed on
    // unchanged, so the Portal never has to know an event's schema.
    payload: raw.payload,
  };
}

/**
 * Strict like `readSessionChange`: a bad kind or an empty id must be refused
 * here, because a permissive target would make the Portal log a delivery
 * failure for an event that was malformed on the wire (ADR-0008).
 */
function readEventTarget(
  raw: Record<string, unknown>,
  messageType: string,
  field: string,
): ServerEventTarget {
  const record = readRecord(raw, messageType, field);
  const prefix = `${messageType}.${field}`;
  const kind = record.kind;
  if (kind === 'connection') {
    return {
      kind: 'connection',
      connectionId: readString(record, prefix, 'connectionId', true),
    };
  }
  if (kind === 'session') {
    return {
      kind: 'session',
      sessionId: readString(record, prefix, 'sessionId', true),
    };
  }
  throw new ShapeError(
    fieldReason(prefix, 'kind', '"connection" or "session"', kind),
  );
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
  const checked = checkWireCapabilities(raw[field], path);
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

/**
 * Absence is allowed, an empty string is not (SPEC 7.3). The type check stays
 * separate so a non-string still reports "a string" rather than the empty-id
 * wording; `isValidClientRequestId` owns only the empty-string rule.
 */
function readClientRequestId(
  raw: Record<string, unknown>,
  messageType: string,
  field: string,
): string | undefined {
  if (raw[field] === undefined) {
    return undefined;
  }
  const value = readString(raw, messageType, field, false);
  if (!isValidClientRequestId(value)) {
    throw new ShapeError(
      fieldReason(messageType, field, 'a non-empty string', value),
    );
  }
  return value;
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
