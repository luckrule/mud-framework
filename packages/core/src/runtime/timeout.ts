/**
 * Caps `overrideMs` by `ceilingMs`. Only a smaller usable value wins: the
 * ceiling is the promise already made — the dispatcher's configured timeout or
 * this executor's established portal deadline — so a caller-supplied override
 * may tighten it but never relax it (SPEC 3.4).
 *
 * An unusable override (NaN, zero, negative, Infinity) is ignored rather than
 * propagated: `setTimeout(NaN)` fires on the next tick, so a bad override would
 * otherwise fail every request immediately.
 */
export function tightenTimeout(
  ceilingMs: number,
  overrideMs: number | undefined,
): number {
  if (
    overrideMs === undefined ||
    !Number.isFinite(overrideMs) ||
    overrideMs <= 0
  ) {
    return ceilingMs;
  }
  return Math.min(ceilingMs, overrideMs);
}

/**
 * SPEC 3.4: the shorter of the Portal ceiling and the backend's declaration
 * wins. The protocol requires a positive finite `timeoutMs`, so a backend that
 * declares 0 or a negative ceiling degrades to the shortest expressible
 * timeout instead of producing an invalid request message.
 */
export function effectiveTimeoutMs(
  portalTimeoutMs: number,
  declaredMaxDurationMs: number,
): number {
  const effective = Math.min(portalTimeoutMs, declaredMaxDurationMs);
  return Number.isFinite(effective) && effective >= 1 ? effective : 1;
}
