import {
  FrameworkTimeoutError,
  RequestCancelledError,
  UnknownHandlerError,
} from './dispatcher.ts';

/*
 * The wire codes (SPEC 7.2). Each value appears exactly once in this file: the
 * constants below are the only place a code string is written, `WIRE_CODES`
 * derives from them and `FRAMEWORK_CODES` derives from that, so adding a code
 * is one edit and the classification follows without a second list.
 *
 * The values themselves are the contract: renaming one is a breaking change
 * for every downstream that keys on it, and a new meaning needs a new name
 * rather than a reused one (INV-014, SPEC 7.2).
 */

/** SPEC 7.2: the request exceeded its deadline. */
export const FRAMEWORK_TIMEOUT = 'FRAMEWORK_TIMEOUT';
/** SPEC 7.2/3.5: the input queue was full, so the request was refused. */
export const FRAMEWORK_QUEUE_FULL = 'FRAMEWORK_QUEUE_FULL';
/** SPEC 7.2/6.3: the framework is stopping; the request was not executed. */
export const FRAMEWORK_SHUTDOWN = 'FRAMEWORK_SHUTDOWN';
/** SPEC 7.2/6.5: the replay budget ran out; the request reached a terminal state. */
export const FRAMEWORK_REPLAY_EXHAUSTED = 'FRAMEWORK_REPLAY_EXHAUSTED';
/** SPEC 7.2/4.3: a reserved code, no path produces it yet. */
export const FRAMEWORK_CAPABILITY_UNSUPPORTED =
  'FRAMEWORK_CAPABILITY_UNSUPPORTED';
/** SPEC 7.2: the transport closed before the request finished. */
export const TRANSPORT_CLOSED = 'TRANSPORT_CLOSED';
/** SPEC 7.2: the result could not be written back to the transport. */
export const TRANSPORT_SEND_FAILED = 'TRANSPORT_SEND_FAILED';
/** SPEC 7.2: a reserved code, no path produces it yet. */
export const STORAGE_UNAVAILABLE = 'STORAGE_UNAVAILABLE';
/** SPEC 7.2: an inbound frame could not be decoded. */
export const INVALID_ENVELOPE = 'INVALID_ENVELOPE';
/** SPEC 7.2: no handler is registered for the method. */
export const UNKNOWN_METHOD = 'UNKNOWN_METHOD';
/** SPEC 7.2: a framework-internal failure, including an undeclared business error. */
export const INTERNAL_ERROR = 'INTERNAL_ERROR';

/**
 * Every wire code, in SPEC 7.2 order. The array is derived from the constants
 * above rather than repeating their values, and is what tests and the
 * classification set iterate over.
 */
export const WIRE_CODES = [
  FRAMEWORK_TIMEOUT,
  FRAMEWORK_QUEUE_FULL,
  FRAMEWORK_SHUTDOWN,
  FRAMEWORK_REPLAY_EXHAUSTED,
  FRAMEWORK_CAPABILITY_UNSUPPORTED,
  TRANSPORT_CLOSED,
  TRANSPORT_SEND_FAILED,
  STORAGE_UNAVAILABLE,
  INVALID_ENVELOPE,
  UNKNOWN_METHOD,
  INTERNAL_ERROR,
] as const;

/**
 * A handler failure whose code is part of the Extension's own contract. Only
 * errors of this type (and the framework's own) decide the wire code: a driver
 * error such as `ENOENT` or `SQLITE_CONSTRAINT_UNIQUE` must not leak into it,
 * because downstream alerts key on those codes (INV-014, SPEC 7.2).
 */
export class BusinessError extends Error {
  readonly code: string;

  constructor(code: string, message: string) {
    super(message);
    this.name = 'BusinessError';
    this.code = code;
  }
}

/**
 * The framework's own wire codes, listed one by one rather than matched by
 * prefix: a business code may legitimately be called anything, including
 * something starting with `FRAMEWORK_`, and a prefix rule would silently
 * downgrade a real failure to a warning. Both the Portal (`IpcExecutor`, which
 * only receives `{code, message}` over IPC) and the Server (`server-process`,
 * which also has `kind`) classify through this one function, so the two
 * processes cannot disagree.
 */
const FRAMEWORK_CODES: ReadonlySet<string> = new Set(WIRE_CODES);

/** Whether `code` is a framework terminal state rather than a business refusal. */
export function isFrameworkCode(code: string): boolean {
  return FRAMEWORK_CODES.has(code);
}

/**
 * The log level a failed request deserves (SPEC 7.1): a business refusal is an
 * expected outcome and belongs at warn, everything else is a failure at error.
 *
 * Used by both processes so they cannot disagree. The list is the judgement:
 * an unrecognised code came from `BusinessError` (the only path that puts a
 * non-framework code on the wire, SPEC 7.2), so it is a business refusal.
 */
export function failureLogLevel(error: { code: string }): 'error' | 'warn' {
  return isFrameworkCode(error.code) ? 'error' : 'warn';
}

/**
 * Only the framework's own errors and an explicit `BusinessError` decide the
 * wire code; anything else is an internal failure. Duck-typing `error.code`
 * would let a driver code such as `ENOENT` become part of the protocol.
 */
export function describeFailure(error: unknown): {
  code: string;
  message: string;
} {
  const message = error instanceof Error ? error.message : String(error);
  if (
    error instanceof FrameworkTimeoutError ||
    error instanceof RequestCancelledError ||
    error instanceof UnknownHandlerError
  ) {
    return { code: error.code, message };
  }
  // An empty code cannot be classified by a client, so it degrades too.
  if (error instanceof BusinessError && error.code.length > 0) {
    return { code: error.code, message };
  }
  return { code: INTERNAL_ERROR, message };
}
