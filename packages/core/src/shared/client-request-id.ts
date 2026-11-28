/**
 * Validation for the optional client correlation key (SPEC 7.3).
 *
 * Absence is the only way to say "the caller supplied none": an empty string
 * would make a present-but-empty id indistinguishable from a missing one, so
 * every entry that accepts the field rejects it. The wording lives here so the
 * dispatcher, the IPC executor and the IPC parser refuse it identically.
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
