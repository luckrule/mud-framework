/**
 * How a raced promise ended. `rejected` is a distinct outcome rather than a
 * boolean, because the callers disagree on what a rejection means: a shutdown
 * step that rejected is a logged failure, while a waiter that only ever
 * resolves treats a rejection as "no longer pending".
 */
export type SettleResult<T> =
  | { status: 'settled'; value: T }
  | { status: 'rejected'; error: unknown }
  | { status: 'timeout' };

/**
 * Races `promise` against `timeoutMs`, reporting which side won.
 *
 * Both outcomes of `promise` are handled here, so a step that loses the race —
 * including one that rejects afterwards — cannot become an unhandled rejection.
 * The timer is cleared in `finally`, so a promise that settles first leaves no
 * pending timer behind (one that fires later would keep the event loop alive).
 *
 * The caller decides what `rejected` means; this function only reports it.
 */
export async function settleWithin<T>(
  promise: Promise<T>,
  ms: number,
): Promise<SettleResult<T>> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const deadline = new Promise<SettleResult<T>>((resolve) => {
    timer = setTimeout(() => {
      resolve({ status: 'timeout' });
    }, ms);
  });
  const settled = promise.then(
    (value): SettleResult<T> => ({ status: 'settled', value }),
    (error: unknown): SettleResult<T> => ({ status: 'rejected', error }),
  );
  try {
    return await Promise.race([settled, deadline]);
  } finally {
    if (timer !== undefined) {
      clearTimeout(timer);
    }
  }
}

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
