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
