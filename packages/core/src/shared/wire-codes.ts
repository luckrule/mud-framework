/*
 * The wire codes (SPEC 7.2). `CODES` is the only place a code string is
 * written: the named exports and `WIRE_CODES` both derive from it, so adding a
 * code is one edit and the classification follows without a second list.
 *
 * The key order is SPEC 7.2 order, and it is also `WIRE_CODES` order: JS
 * preserves insertion order for string keys, so `Object.values` yields the
 * documented order without a separate ordered list to keep in sync.
 *
 * The values themselves are the contract: renaming one is a breaking change
 * for every downstream that keys on it, and a new meaning needs a new name
 * rather than a reused one (INV-014, SPEC 7.2).
 */
const CODES = {
  /** SPEC 7.2: the request exceeded its deadline. */
  FRAMEWORK_TIMEOUT: 'FRAMEWORK_TIMEOUT',
  /** SPEC 7.2/3.5: the input queue was full, so the request was refused. */
  FRAMEWORK_QUEUE_FULL: 'FRAMEWORK_QUEUE_FULL',
  /** SPEC 7.2/6.3: the framework is stopping; execution outcome is not guaranteed. */
  FRAMEWORK_SHUTDOWN: 'FRAMEWORK_SHUTDOWN',
  /** SPEC 7.2/6.5: the replay budget ran out; the request reached a terminal state. */
  FRAMEWORK_REPLAY_EXHAUSTED: 'FRAMEWORK_REPLAY_EXHAUSTED',
  /** SPEC 7.2: the transport closed before the request finished. */
  TRANSPORT_CLOSED: 'TRANSPORT_CLOSED',
  /** SPEC 7.2: the result could not be written back to the transport. */
  TRANSPORT_SEND_FAILED: 'TRANSPORT_SEND_FAILED',
  /** SPEC 7.2: an inbound frame could not be decoded. */
  INVALID_ENVELOPE: 'INVALID_ENVELOPE',
  /** SPEC 7.2: no handler is registered for the method. */
  UNKNOWN_METHOD: 'UNKNOWN_METHOD',
  /** SPEC 7.2: a framework-internal failure, including an undeclared business error. */
  INTERNAL_ERROR: 'INTERNAL_ERROR',
  /** SPEC 7.2/3.7: the handler requires a Session the request did not carry. */
  AUTH_REQUIRED: 'AUTH_REQUIRED',
  /** SPEC 7.2/3.7: the injected access policy refused the request. */
  FORBIDDEN: 'FORBIDDEN',
} as const;

export const {
  FRAMEWORK_TIMEOUT,
  FRAMEWORK_QUEUE_FULL,
  FRAMEWORK_SHUTDOWN,
  FRAMEWORK_REPLAY_EXHAUSTED,
  TRANSPORT_CLOSED,
  TRANSPORT_SEND_FAILED,
  INVALID_ENVELOPE,
  UNKNOWN_METHOD,
  INTERNAL_ERROR,
  AUTH_REQUIRED,
  FORBIDDEN,
} = CODES;

/**
 * Every wire code, in SPEC 7.2 order (`CODES` key order). What tests and the
 * classification set iterate over.
 */
export const WIRE_CODES: readonly string[] = Object.values(CODES);

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

export class FrameworkTimeoutError extends Error {
  readonly code = FRAMEWORK_TIMEOUT;
  readonly requestId: string;
  readonly timeoutMs: number;

  constructor(requestId: string, timeoutMs: number) {
    super(`request ${requestId} timed out after ${timeoutMs}ms`);
    this.name = 'FrameworkTimeoutError';
    this.requestId = requestId;
    this.timeoutMs = timeoutMs;
  }
}

/**
 * A request ended because someone asked it to, not because it ran out of time.
 * A distinct type so the code stays the caller's (a connection layer sends
 * `TRANSPORT_CLOSED`, a Portal sends `FRAMEWORK_TIMEOUT`), rather than being
 * flattened into the timeout code (SPEC 7.2).
 */
export class RequestCancelledError extends Error {
  readonly code: string;
  readonly requestId: string;

  constructor(requestId: string, code: string) {
    super(`request ${requestId} was cancelled: ${code}`);
    this.name = 'RequestCancelledError';
    this.code = code;
    this.requestId = requestId;
  }
}

/**
 * A distinct type rather than a bare Error: the connection layer maps it to a
 * stable wire code, and callers must not have to match on message text to tell
 * "no such method" apart from a handler that failed (INV-014).
 */
export class UnknownHandlerError extends Error {
  readonly code = UNKNOWN_METHOD;
  readonly method: string;

  constructor(method: string) {
    super(`no handler registered for "${method}"`);
    this.name = 'UnknownHandlerError';
    this.method = method;
  }
}

/**
 * The handler's declared access requires a Session, but the request entered
 * without one (SPEC 3.7). A distinct type so the wire code does not depend on
 * matching message text (INV-014).
 */
export class AuthenticationRequiredError extends Error {
  readonly code = AUTH_REQUIRED;
  readonly method: string;

  constructor(method: string) {
    super(`handler "${method}" requires a session`);
    this.name = 'AuthenticationRequiredError';
    this.method = method;
  }
}

/**
 * The injected access policy refused this request (SPEC 3.7). A framework code
 * rather than a business one: the policy belongs to the consumer, but the
 * refusal semantics and the wire code are the framework's.
 */
export class ForbiddenError extends Error {
  readonly code = FORBIDDEN;
  readonly method: string;

  constructor(method: string) {
    super(`access to "${method}" was denied`);
    this.name = 'ForbiddenError';
    this.method = method;
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

/**
 * Framework codes that are expected refusals rather than failures (SPEC 7.1,
 * 7.2). They stay framework codes for classification, but a logged request
 * ending in one of them must not be recorded at error level.
 */
const EXPECTED_REFUSALS: ReadonlySet<string> = new Set([
  AUTH_REQUIRED,
  FORBIDDEN,
]);

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
 * `AUTH_REQUIRED` and `FORBIDDEN` are framework codes but expected refusals, so
 * they need the explicit set above rather than the framework/business split.
 */
export function failureLogLevel(error: { code: string }): 'error' | 'warn' {
  if (EXPECTED_REFUSALS.has(error.code)) {
    return 'warn';
  }
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
    error instanceof UnknownHandlerError ||
    error instanceof AuthenticationRequiredError ||
    error instanceof ForbiddenError
  ) {
    return { code: error.code, message };
  }
  // An empty code cannot be classified by a client, so it degrades too.
  if (error instanceof BusinessError && error.code.length > 0) {
    return { code: error.code, message };
  }
  return { code: INTERNAL_ERROR, message };
}
