/**
 * Internal helpers shared across `ipc/`, `logging/`, `server/`, `shared/`,
 * `storage/` and `portal/`: value description, the numeric guards for configured
 * options and the client correlation key validation. They live together so every
 * rejection names the received value the same way and accepted ranges cannot
 * drift apart between callers.
 *
 * Deliberately not re-exported from `index.ts`: these are implementation
 * details of the core, and widening the public surface to include them would
 * turn them into contract. `shared/` is the one place no other directory has
 * to depend on a sibling directory to reach.
 */

/**
 * Every validator here receives `unknown`, and almost all of them need to read
 * properties only when the value is a plain object; `typeof` alone accepts
 * arrays and `null`, which would then be probed for fields.
 */
export function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

/**
 * Rejection reasons must say what was received, but the value may hold client
 * data, so only its type and scalar text are reported, never a serialized copy.
 */
export function describeValue(value: unknown): string {
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

/**
 * Catch blocks bind `unknown`, yet every error path here needs a string for a
 * log line or an error envelope; a non-`Error` throw still has to produce one.
 */
export function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

/**
 * A value thrown by outside code, as an `Error`. Callers need `instanceof` or a
 * message, and a rejection may carry anything; a non-`Error` throw keeps the
 * text `messageOf` gives it.
 */
export function toError(error: unknown): Error {
  return error instanceof Error ? error : new Error(messageOf(error));
}

/**
 * Largest delay `setTimeout` accepts. Bun clamps anything above it to 1ms, so a
 * derived bound that overflowed this would release the writer almost at once —
 * the opposite of what the bound is for (SPEC 3.8). Shared by production
 * validation and the option tests so both name the same ceiling.
 */
export const MAX_TIMER_DELAY_MS = 2_147_483_647;

/** Caps a derived delay where `setTimeout` stops honouring it. */
export function clampTimerDelay(ms: number): number {
  return Math.min(ms, MAX_TIMER_DELAY_MS);
}

/**
 * A count that must be a whole number of at least 1. A fractional or infinite
 * value is rejected rather than rounded or capped: the caller stated a bound
 * and the framework must not silently apply a different one.
 */
export function assertPositiveInteger(value: unknown, name: string): void {
  if (typeof value !== 'number') {
    throw new TypeError(
      `${name} must be a positive integer, got ${describeValue(value)}`,
    );
  }
  if (!Number.isInteger(value) || value < 1) {
    throw new RangeError(
      `${name} must be a positive integer, got ${String(value)}`,
    );
  }
}

/**
 * Same contract for counts that also feed arithmetic, where `MAX_SAFE_INTEGER`
 * is the real ceiling: beyond it `+ 1` no longer changes the number.
 *
 * `ceiling` adds an upper bound the message must name (the timer ceiling, say).
 * The comparison and the printed label are separate because the caller's label
 * may carry a parenthetical the bare number does not.
 */
export function assertPositiveSafeInteger(
  value: unknown,
  name: string,
  ceiling?: { readonly max: number; readonly label: string },
): asserts value is number {
  const expected =
    ceiling === undefined
      ? 'a safe integer of at least 1'
      : `a safe integer between 1 and ${ceiling.label}`;
  if (typeof value !== 'number') {
    throw new TypeError(
      `${name} must be ${expected}, got ${describeValue(value)}`,
    );
  }
  if (
    !Number.isSafeInteger(value) ||
    value < 1 ||
    (ceiling !== undefined && value > ceiling.max)
  ) {
    throw new RangeError(`${name} must be ${expected}, got ${String(value)}`);
  }
}

/**
 * A configured delay must also fit `setTimeout`. Above `MAX_TIMER_DELAY_MS` Bun
 * clamps the delay to 1ms, so the longest wait an operator can ask for becomes
 * the shortest: shutdown would drop every queued request, and a send bound would
 * release the writer while the frame may still be on the wire. Rejected rather
 * than capped because the value is the operator's stated intent; silently
 * shortening it is the same inversion, and a delay this far out of range is a
 * configuration error worth reporting rather than quietly repairing.
 *
 * Every caller of this is a timer delay, so the ceiling applies to all of them.
 * A fractional delay is accepted here; callers that need whole milliseconds add
 * that check themselves (see `assertExecutorTimerMs`).
 */
export function assertTimerDelay(
  value: unknown,
  name: string,
): asserts value is number {
  if (typeof value !== 'number') {
    throw new TypeError(
      `${name} must be a finite number of at least 1, got ${describeValue(value)}`,
    );
  }
  if (!Number.isFinite(value) || value < 1) {
    throw new RangeError(
      `${name} must be a finite number of at least 1, got ${String(value)}`,
    );
  }
  if (value > MAX_TIMER_DELAY_MS) {
    throw new RangeError(
      `${name} must not exceed ${MAX_TIMER_DELAY_MS}ms (the largest setTimeout delay), got ${String(value)}`,
    );
  }
}

/**
 * Validation for the client correlation key (SPEC 7.3).
 *
 * The field is absent only when a direct `Dispatcher.dispatch` or an IPC
 * `request` message omitted it; a request that entered through the client wire
 * format always carries one (`Envelope.id` is required non-empty). Absence is
 * the only valid way to say "none": an empty string would be indistinguishable
 * from a missing id, so every entry that accepts the field rejects it. The
 * wording lives here so the dispatcher and the IPC executor refuse it
 * identically; the parser uses the same predicate and wraps it in its own
 * `ShapeError`.
 */
export const CLIENT_REQUEST_ID_EMPTY_MESSAGE =
  'clientRequestId must be a non-empty string when provided';

/**
 * Non-throwing form for callers with their own error channel: the parser builds
 * a `ShapeError` from it and `IpcExecutor` returns an `INTERNAL_ERROR` outcome
 * instead of throwing.
 */
export function isValidClientRequestId(value: string | undefined): boolean {
  return value === undefined || value.length > 0;
}

/** Throws `TypeError` for the one invalid value an optional id admits. */
export function assertClientRequestId(value: string | undefined): void {
  if (!isValidClientRequestId(value)) {
    throw new TypeError(CLIENT_REQUEST_ID_EMPTY_MESSAGE);
  }
}
