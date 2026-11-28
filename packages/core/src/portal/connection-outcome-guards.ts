import type { ExecutorOutcome, SessionChange } from '../shared/executor.ts';
import { isRecord, messageOf } from '../shared/validation.ts';
import { INTERNAL_ERROR } from '../shared/wire-codes.ts';

/**
 * A guard, not a normaliser. The executor is outside code, so a returned object
 * is not proof of the field the caller reads: a bare `{ok:false}` would throw on
 * `outcome.error.code` after the `execute()` try/catch and drop the request with
 * no frame at all (SPEC 3.5). Repairing a malformed outcome is not this layer's
 * job: anything shaped wrongly is a framework failure, not a client-visible
 * business result.
 */
export function isExecutorOutcome(value: unknown): value is ExecutorOutcome {
  if (!isRecord(value)) {
    return false;
  }
  if (value.ok === true) {
    return (
      value.sessionChange === undefined || isSessionChange(value.sessionChange)
    );
  }
  if (value.ok !== false) {
    return false;
  }
  const error = value.error;
  return (
    isRecord(error) &&
    typeof error.code === 'string' &&
    error.code.length > 0 &&
    typeof error.message === 'string'
  );
}

/**
 * The same strictness as the IPC parser: a malformed change must not reach the
 * router, where a bad id or an unknown type would silently misroute a later
 * request (SPEC 3.7).
 */
export function isSessionChange(value: unknown): value is SessionChange {
  if (!isRecord(value)) {
    return false;
  }
  if (value.type === 'bind') {
    return typeof value.sessionId === 'string' && value.sessionId.length > 0;
  }
  if (value.type === 'unbind') {
    return value.sessionId === undefined;
  }
  return false;
}

/** Maps anything the executor threw, or a malformed result, to the failure path. */
export function toFailure(error: unknown): ExecutorOutcome {
  return {
    ok: false,
    error: { code: INTERNAL_ERROR, message: messageOf(error) },
  };
}
